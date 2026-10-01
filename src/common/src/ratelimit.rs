//! # Per-Tenant Rate Limiting
//!
//! Token-bucket rate limiting keyed by tenant id, used by the ingest
//! paths (OTLP gRPC, Prometheus remote_write) to keep one tenant from
//! exhausting shared acceptor throughput, and by the router's query API
//! to keep one tenant from monopolizing shared query capacity.
//!
//! Limits come from `[auth].default_limits` with optional per-tenant
//! overrides (`[[auth.tenants]].limits`). Unset limits mean unlimited,
//! so deployments without limit configuration are unaffected.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

    /// Refill for elapsed time, then try to take `cost` tokens. On
    /// rejection, `Err` carries the time until `cost` tokens would be
    /// available.
    fn try_acquire(&mut self, cost: f64, now: Instant) -> Result<(), Duration> {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last_refill = now;
        if self.tokens >= cost {
            self.tokens -= cost;
            return Ok(());
        }
        Err(self.wait_for(cost))
    }

    /// Time until `cost` tokens would be available.
    ///
    /// A `cost` larger than the whole burst (only possible for the bytes
    /// dimension, where a single request can outweigh the burst) can never
    /// be admitted by this bucket; rather than report an unbounded wait, we
    /// report the time to fill the whole burst from empty, so the caller
    /// always gets a finite, honest number. A misconfigured zero rate (an
    /// explicit `0` limit, which denies everything) similarly never
    /// refills; report a bounded "effectively never" wait instead of
    /// dividing by zero.
    fn wait_for(&self, cost: f64) -> Duration {
        if self.rate <= 0.0 {
            return Duration::from_secs(u32::MAX as u64);
        }
        let deficit = if cost > self.burst {
            self.burst
        } else {
            (cost - self.tokens).max(0.0)
        };
        Duration::from_secs_f64(deficit / self.rate)
    }
}

/// Buckets for one tenant; `None` means that dimension is unlimited.
#[derive(Debug, Default)]
struct TenantBuckets {
    requests: Option<TokenBucket>,
    bytes: Option<TokenBucket>,
    query_requests: Option<TokenBucket>,
}

/// The dimension that rejected a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitKind {
    /// The per-tenant requests/second limit.
    Requests,
    /// The per-tenant ingest bytes/second limit.
    Bytes,
    /// The per-tenant query API requests/second limit.
    QueryRequests,
}

impl RateLimitKind {
    /// Low-cardinality label value for metrics (`kind` on
    /// `signaldb_rate_limit_rejections_total`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Bytes => "bytes",
            Self::QueryRequests => "query_requests",
        }
    }
}

/// Error returned when a tenant exceeds an ingest or query limit.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimitExceeded {
    pub tenant_id: String,
    pub kind: RateLimitKind,
    /// Time until the rejected request would be admitted, computed from the
    /// bucket's actual state (see [`TokenBucket::wait_for`]).
    pub retry_after: Duration,
    /// The dimension's per-second budget (tokens/second).
    pub limit: f64,
    /// The dimension's burst allowance (tokens).
    pub burst: f64,
}

impl RateLimitExceeded {
    /// `Retry-After` value: whole seconds, rounded up, never below 1 — the
    /// contract every SignalDB 429 honors so a client has one retry rule
    /// regardless of which surface rejected it.
    pub fn retry_after_secs(&self) -> u64 {
        (self.retry_after.as_secs_f64().ceil() as u64).max(1)
    }
}

impl std::fmt::Display for RateLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            RateLimitKind::Requests => "request rate",
            RateLimitKind::Bytes => "ingest byte rate",
            RateLimitKind::QueryRequests => "query request rate",
        };
        write!(
            f,
            "tenant '{}' exceeded its {what} limit; retry after {}s or raise the tenant's limits",
            self.tenant_id,
            self.retry_after_secs()
        )
    }
}

impl std::error::Error for RateLimitExceeded {}

/// Acquire `cost` tokens from `bucket` (a no-op when the dimension has no
/// configured limit, i.e. `bucket` is `None`), mapping a rejection to
/// [`RateLimitExceeded`] for `kind`. Shared by every per-dimension check in
/// [`TenantRateLimiter::check_ingest_at`] and
/// [`TenantRateLimiter::check_query_at`].
fn try_acquire(
    bucket: Option<&mut TokenBucket>,
    cost: f64,
    now: Instant,
    tenant_id: &str,
    kind: RateLimitKind,
) -> Result<(), RateLimitExceeded> {
    let Some(bucket) = bucket else {
        return Ok(());
    };
    bucket
        .try_acquire(cost, now)
        .map_err(|retry_after| RateLimitExceeded {
            tenant_id: tenant_id.to_string(),
            kind,
            retry_after,
            limit: bucket.rate,
            burst: bucket.burst,
        })
}

/// The `Retry-After` / `X-RateLimit-Limit` / `X-RateLimit-Burst` header trio
/// every SignalDB 429 carries, computed from the rejected bucket's actual
/// state. Shared by the router and the acceptor so both surfaces answer with
/// identical headers (`http` crate types, which both already depend on via
/// `axum`).
pub fn retry_headers(
    err: &RateLimitExceeded,
) -> [(axum::http::HeaderName, axum::http::HeaderValue); 3] {
    [
        (
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from(err.retry_after_secs()),
        ),
        (
            axum::http::HeaderName::from_static("x-ratelimit-limit"),
            header_value_from_budget(err.limit),
        ),
        (
            axum::http::HeaderName::from_static("x-ratelimit-burst"),
            header_value_from_budget(err.burst),
        ),
    ]
}

/// Render a token-bucket budget (rate or burst, always a non-negative whole
/// number of requests or bytes in practice) as a header value. Falls back to
/// `"0"` rather than panicking on the unreachable case of a non-finite
/// value, per the no-`unwrap`-in-production-paths rule.
fn header_value_from_budget(value: f64) -> axum::http::HeaderValue {
    let rounded = if value.is_finite() {
        value.max(0.0)
    } else {
        0.0
    };
    axum::http::HeaderValue::from(rounded as u64)
}

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

        let entry = self.bucket_entry(tenant_id, now);
        let mut buckets = entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        try_acquire(
            buckets.requests.as_mut(),
            1.0,
            now,
            tenant_id,
            RateLimitKind::Requests,
        )?;
        try_acquire(
            buckets.bytes.as_mut(),
            bytes as f64,
            now,
            tenant_id,
            RateLimitKind::Bytes,
        )?;
        Ok(())
    }

    /// Record one query API request for the tenant, rejecting it if the
    /// query request-rate budget is exhausted.
    pub fn check_query(&self, tenant_id: &str) -> Result<(), RateLimitExceeded> {
        self.check_query_at(tenant_id, Instant::now())
    }

    /// [`Self::check_query`] with an injectable clock for tests.
    fn check_query_at(&self, tenant_id: &str, now: Instant) -> Result<(), RateLimitExceeded> {
        let limits = self.limits_for(tenant_id);
        if limits.max_query_requests_per_sec.is_none() {
            return Ok(());
        }

        let entry = self.bucket_entry(tenant_id, now);
        let mut buckets = entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        try_acquire(
            buckets.query_requests.as_mut(),
            1.0,
            now,
            tenant_id,
            RateLimitKind::QueryRequests,
        )?;
        Ok(())
    }

    /// The tenant's bucket set, created from its limits on first use.
    fn bucket_entry(
        &self,
        tenant_id: &str,
        now: Instant,
    ) -> dashmap::mapref::one::RefMut<'_, String, Mutex<TenantBuckets>> {
        let limits = self.limits_for(tenant_id);
        self.buckets
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
                    query_requests: limits.max_query_requests_per_sec.map(|qps| {
                        let rate = f64::from(qps);
                        TokenBucket::new(rate, rate * burst_secs, now)
                    }),
                })
            })
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
    fn query_rate_is_enforced_and_refills() {
        let limiter = limiter(
            TenantLimits {
                max_query_requests_per_sec: Some(4),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );

        let start = Instant::now();
        for _ in 0..4 {
            assert!(limiter.check_query_at("acme", start).is_ok());
        }
        let denied = limiter.check_query_at("acme", start).unwrap_err();
        assert_eq!(denied.kind, RateLimitKind::QueryRequests);

        // Half a second refills two tokens.
        let later = start + Duration::from_millis(500);
        for _ in 0..2 {
            assert!(limiter.check_query_at("acme", later).is_ok());
        }
        assert!(limiter.check_query_at("acme", later).is_err());
    }

    #[test]
    fn query_rate_unlimited_when_unset_even_with_ingest_limits() {
        let limiter = limiter(
            TenantLimits {
                max_ingest_requests_per_sec: Some(1),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );

        let start = Instant::now();
        // Exhaust the ingest budget; queries must be unaffected.
        assert!(limiter.check_ingest_at("acme", 0, start).is_ok());
        assert!(limiter.check_ingest_at("acme", 0, start).is_err());
        for _ in 0..1_000 {
            assert!(limiter.check_query_at("acme", start).is_ok());
        }
    }

    #[test]
    fn query_and_ingest_budgets_are_independent() {
        let limiter = limiter(
            TenantLimits {
                max_ingest_requests_per_sec: Some(2),
                max_query_requests_per_sec: Some(2),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );

        let start = Instant::now();
        assert!(limiter.check_query_at("acme", start).is_ok());
        assert!(limiter.check_query_at("acme", start).is_ok());
        assert!(limiter.check_query_at("acme", start).is_err());

        // The exhausted query budget must not consume ingest tokens.
        assert!(limiter.check_ingest_at("acme", 0, start).is_ok());
        assert!(limiter.check_ingest_at("acme", 0, start).is_ok());
        assert!(limiter.check_ingest_at("acme", 0, start).is_err());
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

    #[test]
    fn try_acquire_reports_wait_as_deficit_over_rate() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(4.0, 4.0, start);
        for _ in 0..4 {
            assert!(bucket.try_acquire(1.0, start).is_ok());
        }
        // Empty bucket, rate 4/s: 1 more token needs 0.25s.
        let wait = bucket.try_acquire(1.0, start).unwrap_err();
        assert!((wait.as_secs_f64() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn try_acquire_reports_full_burst_fill_time_when_cost_exceeds_burst() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(100.0, 500.0, start);
        // A single request costing more than the whole burst (e.g. an
        // oversized ingest payload) can never be admitted; the wait is the
        // time to fill the burst from empty, not an unbounded number.
        let wait = bucket.try_acquire(1_000.0, start).unwrap_err();
        assert!((wait.as_secs_f64() - 5.0).abs() < 1e-9);
    }

    #[test]
    fn rate_limit_exceeded_carries_wait_limit_and_burst() {
        let limiter = limiter(
            TenantLimits {
                max_query_requests_per_sec: Some(4),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );
        let start = Instant::now();
        for _ in 0..4 {
            assert!(limiter.check_query_at("acme", start).is_ok());
        }
        let denied = limiter.check_query_at("acme", start).unwrap_err();
        assert_eq!(denied.kind, RateLimitKind::QueryRequests);
        assert_eq!(denied.limit, 4.0);
        assert_eq!(denied.burst, 4.0);
        assert!((denied.retry_after.as_secs_f64() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn retry_after_secs_rounds_up_and_is_never_below_one() {
        let base = RateLimitExceeded {
            tenant_id: "acme".to_string(),
            kind: RateLimitKind::QueryRequests,
            retry_after: Duration::from_millis(10),
            limit: 10.0,
            burst: 10.0,
        };
        assert_eq!(base.retry_after_secs(), 1);

        let longer = RateLimitExceeded {
            retry_after: Duration::from_millis(1_500),
            ..base.clone()
        };
        assert_eq!(longer.retry_after_secs(), 2);

        let zero = RateLimitExceeded {
            retry_after: Duration::ZERO,
            ..base
        };
        assert_eq!(zero.retry_after_secs(), 1);
    }

    #[test]
    fn retry_headers_carry_the_bucket_state() {
        let err = RateLimitExceeded {
            tenant_id: "acme".to_string(),
            kind: RateLimitKind::QueryRequests,
            retry_after: Duration::from_millis(2_500),
            limit: 10.0,
            burst: 100.0,
        };
        let headers = retry_headers(&err);
        let find = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n.as_str() == name)
                .map(|(_, v)| v.to_str().unwrap().to_string())
        };
        assert_eq!(find("retry-after").as_deref(), Some("3"));
        assert_eq!(find("x-ratelimit-limit").as_deref(), Some("10"));
        assert_eq!(find("x-ratelimit-burst").as_deref(), Some("100"));
    }
}
