//! # Retry on throttle
//!
//! The shared retry policy every generated operation runs through (see the
//! `client-retry-on-throttle` spec). `cargo xtask generate` installs
//! [`execute`] as the progenitor `ClientHooks::exec` override of the generated
//! [`Client`](crate::Client), and carries the [`RetryPolicy`] as the client's
//! inner value, so no call site needs to know retry exists.
//!
//! ## Contract (identical to the TypeScript client, `src/ui/src/api/http.ts`)
//!
//! - `429` is retried on any method — a throttled request was not processed.
//! - `502`/`503`/`504`, connection failures, and request timeouts are retried
//!   only for idempotent methods (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`,
//!   `TRACE`), because the server may have acted on a `POST`.
//! - Every other outcome is returned as-is.
//! - The wait honours `Retry-After` (seconds or HTTP-date); otherwise it is a
//!   fully jittered exponential backoff (`U(0, min(cap, base·2^n))`).
//! - A `Retry-After` beyond [`RetryPolicy::per_attempt_cap`], or a total wait
//!   that would exceed [`RetryPolicy::total_cap`], fails fast: the throttling
//!   response is returned immediately so callers learn at once.
//! - Attempts are bounded by [`RetryPolicy::max_attempts`] (the total number
//!   of requests sent, including the first).
//!
//! The table in `api/retry-cases.json` is the executable form of these rules;
//! both language test suites replay it.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use progenitor_client::OperationInfo;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Method, StatusCode};

/// What a client should do about one attempt's outcome. Produced by
/// [`RetryPolicy::decide`] and consumed by [`execute`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Return the outcome to the caller as-is (success, non-retryable
    /// failure, or the attempt budget is spent).
    Return,
    /// Retry after waiting this long.
    Retry(Duration),
    /// The failure was retryable but the wait the server asked for is
    /// beyond the policy's ceiling; return it now rather than blocking.
    FailFast,
}

/// The retryable-or-not shape of one attempt, independent of transport types
/// so the shared fixture can drive [`RetryPolicy::decide`] directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The server answered with this status.
    Status(u16),
    /// The connection could not be established (or was reset before a
    /// response arrived).
    Connect,
    /// The request timed out.
    Timeout,
}

/// One retry, as reported to [`RetryPolicy::on_retry`] and to the
/// `signaldb_sdk::retry` tracing target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryEvent {
    /// The attempt number that just failed (1 = the first request).
    pub attempt: u32,
    /// How long the client waits before the next attempt.
    pub wait: Duration,
    /// What failed.
    pub failure: Failure,
}

/// A caller-supplied observer invoked once per retry (the CLI uses it for
/// its interactive "waiting…" note).
pub type RetryObserver = Arc<dyn Fn(&RetryEvent) + Send + Sync>;

/// The retry policy of a client. Construct with [`RetryPolicy::default`] (the
/// SignalDB defaults) or [`RetryPolicy::disabled`], then adjust fields.
#[derive(Clone)]
pub struct RetryPolicy {
    /// Total attempts including the first request. `1` disables retry.
    pub max_attempts: u32,
    /// Base of the jittered exponential backoff used without `Retry-After`.
    pub base: Duration,
    /// Ceiling on any single wait; a `Retry-After` above it fails fast.
    pub per_attempt_cap: Duration,
    /// Ceiling on the sum of all waits for one call.
    pub total_cap: Duration,
    /// Whether `502`/`503`/`504`, connection failures, and timeouts are
    /// retried for idempotent methods. `429` is retried regardless.
    pub retry_transient_idempotent: bool,
    /// Optional observer called once per retry, before the wait.
    pub on_retry: Option<RetryObserver>,
}

impl std::fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_attempts", &self.max_attempts)
            .field("base", &self.base)
            .field("per_attempt_cap", &self.per_attempt_cap)
            .field("total_cap", &self.total_cap)
            .field(
                "retry_transient_idempotent",
                &self.retry_transient_idempotent,
            )
            .field("on_retry", &self.on_retry.as_ref().map(|_| "…"))
            .finish()
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base: Duration::from_millis(250),
            per_attempt_cap: Duration::from_secs(10),
            total_cap: Duration::from_secs(30),
            retry_transient_idempotent: true,
            on_retry: None,
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries: every failure, including `429`, is
    /// reported on the first attempt.
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    /// Whether this policy can ever retry.
    pub fn is_enabled(&self) -> bool {
        self.max_attempts > 1
    }

    /// Attach a per-retry observer.
    pub fn with_observer(mut self, observer: RetryObserver) -> Self {
        self.on_retry = Some(observer);
        self
    }

    /// Decide what to do after `attempt` (1-based) ended in `failure`.
    ///
    /// `retry_after` is the server-stated wait, if any; `waited` is the sum
    /// of waits already spent on this call.
    pub fn decide(
        &self,
        failure: Failure,
        method: &Method,
        retry_after: Option<Duration>,
        attempt: u32,
        waited: Duration,
    ) -> Decision {
        if !self.is_retryable(failure, method) {
            return Decision::Return;
        }
        if attempt >= self.max_attempts {
            return Decision::Return;
        }
        let wait = match retry_after {
            Some(server_wait) => {
                if server_wait > self.per_attempt_cap {
                    return Decision::FailFast;
                }
                server_wait
            }
            None => self.backoff(attempt),
        };
        if waited.saturating_add(wait) > self.total_cap {
            return Decision::FailFast;
        }
        Decision::Retry(wait)
    }

    /// Whether `failure` on `method` is retryable at all under this policy.
    pub fn is_retryable(&self, failure: Failure, method: &Method) -> bool {
        match failure {
            Failure::Status(429) => true,
            Failure::Status(502..=504) | Failure::Connect | Failure::Timeout => {
                self.retry_transient_idempotent && is_idempotent(method)
            }
            Failure::Status(_) => false,
        }
    }

    /// The upper bound of the jittered wait after `attempt` (1-based).
    pub fn backoff_ceiling(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1).min(31);
        self.base
            .checked_mul(1u32 << exp)
            .unwrap_or(self.per_attempt_cap)
            .min(self.per_attempt_cap)
    }

    fn backoff(&self, attempt: u32) -> Duration {
        self.backoff_ceiling(attempt).mul_f64(rand::random::<f64>())
    }
}

/// Idempotent methods per RFC 9110 §9.2.2 (`POST`/`PATCH`/`CONNECT` are not).
pub fn is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}

/// Parse a `Retry-After` header value: delay-seconds or an HTTP-date. An
/// HTTP-date in the past yields zero; anything unparseable yields `None`.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(
        at.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

/// The `Retry-After` of a response header map, if present and parseable.
pub fn retry_after_from_headers(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_retry_after)
}

/// The server-stated wait carried by a final SDK error, so consumers can
/// tell their caller "rate limited; retry in Ns" after retries are spent.
///
/// Reads `Retry-After` from documented (`ErrorResponse`) and undocumented
/// (`UnexpectedResponse`) responses alike.
pub fn retry_after<E>(err: &progenitor_client::Error<E>) -> Option<Duration> {
    match err {
        progenitor_client::Error::ErrorResponse(rv) => retry_after_from_headers(rv.headers()),
        progenitor_client::Error::UnexpectedResponse(resp) => {
            retry_after_from_headers(resp.headers())
        }
        _ => None,
    }
}

/// True when the SDK error is a `429` (retries exhausted, or retry disabled).
pub fn is_throttled<E>(err: &progenitor_client::Error<E>) -> bool {
    err.status() == Some(StatusCode::TOO_MANY_REQUESTS)
}

/// A throttled outcome extracted from an erased error (see
/// [`throttle_of`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Throttled {
    /// The server-stated wait on the final `429`, when it carried one.
    pub retry_after: Option<Duration>,
}

/// Inspect a type-erased error (for example one link of an `anyhow` chain)
/// and, if it is an SDK error whose final status was `429`, return the
/// throttling detail. Every generated error type is covered; a test in
/// `tests/retry.rs` guards the list against the generated file.
pub fn throttle_of(err: &(dyn std::error::Error + 'static)) -> Option<Throttled> {
    macro_rules! probe {
        ($($ty:ty),* $(,)?) => {
            $(
                if let Some(e) = err.downcast_ref::<progenitor_client::Error<$ty>>() {
                    return is_throttled(e).then(|| Throttled { retry_after: retry_after(e) });
                }
            )*
        };
    }
    probe!(
        (),
        crate::types::ApiError,
        crate::types::ManageError,
        crate::types::SchemaError,
    );
    None
}

/// The generated error payload types [`throttle_of`] can see through. Kept
/// next to the macro call above so the drift test can compare it with the
/// generated file.
pub const KNOWN_ERROR_TYPES: &[&str] = &["()", "ApiError", "ManageError", "SchemaError"];

fn failure_of(result: &reqwest::Result<reqwest::Response>) -> Option<Failure> {
    match result {
        Ok(resp) => {
            let status = resp.status();
            (status.is_client_error() || status.is_server_error())
                .then(|| Failure::Status(status.as_u16()))
        }
        Err(e) if e.is_timeout() => Some(Failure::Timeout),
        Err(e) if e.is_connect() || e.is_request() => Some(Failure::Connect),
        Err(_) => None,
    }
}

/// Execute `request` under `policy`: the `ClientHooks::exec` override the
/// generated client routes every operation through.
///
/// A request whose body cannot be cloned (a streaming body) is sent once. The
/// final outcome — success or the last failure — is returned unchanged, so
/// progenitor's own status handling still applies and a final `429` still
/// carries its `Retry-After` for [`retry_after`].
pub async fn execute(
    client: &reqwest::Client,
    policy: &RetryPolicy,
    mut request: reqwest::Request,
    info: &OperationInfo,
) -> reqwest::Result<reqwest::Response> {
    #[cfg(feature = "tracing")]
    trace_context::inject(request.headers_mut());
    let method = request.method().clone();
    let mut attempt: u32 = 1;
    let mut waited = Duration::ZERO;
    loop {
        let next = request.try_clone();
        let result = client.execute(request).await;
        let Some(failure) = failure_of(&result) else {
            return result;
        };
        let Some(next_request) = next else {
            return result;
        };
        let retry_after = result
            .as_ref()
            .ok()
            .and_then(|r| retry_after_from_headers(r.headers()));
        match policy.decide(failure, &method, retry_after, attempt, waited) {
            Decision::Return => return result,
            Decision::FailFast => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: "signaldb_sdk::retry",
                    operation = info.operation_id,
                    attempt,
                    retry_after_ms = retry_after.map(|d| d.as_millis() as u64),
                    ?failure,
                    "not retrying: server-stated wait exceeds the policy ceiling"
                );
                return result;
            }
            Decision::Retry(wait) => {
                let event = RetryEvent {
                    attempt,
                    wait,
                    failure,
                };
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: "signaldb_sdk::retry",
                    operation = info.operation_id,
                    attempt,
                    wait_ms = wait.as_millis() as u64,
                    status = match failure {
                        Failure::Status(s) => Some(s),
                        _ => None,
                    },
                    ?failure,
                    "retrying request"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = info;
                if let Some(observer) = &policy.on_retry {
                    observer(&event);
                }
                tokio::time::sleep(wait).await;
                waited = waited.saturating_add(wait);
                attempt += 1;
                request = next_request;
            }
        }
    }
}

/// W3C trace-context injection: the current `tracing` span's OpenTelemetry
/// context is written into `traceparent`/`tracestate` through the global
/// propagator. Without an OTel layer (or with the default no-op propagator)
/// nothing is added.
#[cfg(feature = "tracing")]
pub mod trace_context {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    struct HeaderInjector<'a>(&'a mut HeaderMap);

    impl opentelemetry::propagation::Injector for HeaderInjector<'_> {
        fn set(&mut self, key: &str, value: String) {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(key.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                self.0.insert(name, value);
            }
        }
    }

    /// Inject the current span's context into `headers`.
    pub fn inject(headers: &mut HeaderMap) {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut HeaderInjector(headers));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_shared_fixture() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 4);
        assert_eq!(p.base, Duration::from_millis(250));
        assert_eq!(p.per_attempt_cap, Duration::from_secs(10));
        assert_eq!(p.total_cap, Duration::from_secs(30));
        assert!(p.retry_transient_idempotent);
        assert!(p.is_enabled());
        assert!(!RetryPolicy::disabled().is_enabled());
    }

    #[test]
    fn retry_after_parses_seconds_and_http_dates() {
        assert_eq!(parse_retry_after("5"), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after(" 0 "), Some(Duration::ZERO));
        assert_eq!(parse_retry_after("soon"), None);
        assert_eq!(parse_retry_after("-1"), None);
        let future = SystemTime::now() + Duration::from_secs(120);
        let parsed = parse_retry_after(&httpdate::fmt_http_date(future)).expect("http-date");
        assert!(parsed > Duration::from_secs(110) && parsed <= Duration::from_secs(120));
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO),
            "a date in the past means retry now"
        );
    }

    #[test]
    fn total_cap_fails_fast_when_the_budget_is_spent() {
        let p = RetryPolicy::default();
        let d = p.decide(
            Failure::Status(429),
            &Method::GET,
            Some(Duration::from_secs(5)),
            1,
            Duration::from_secs(26),
        );
        assert_eq!(d, Decision::FailFast);
    }

    #[test]
    fn disabled_policy_never_retries() {
        let p = RetryPolicy::disabled();
        assert_eq!(
            p.decide(Failure::Status(429), &Method::GET, None, 1, Duration::ZERO),
            Decision::Return
        );
    }

    #[test]
    fn transient_retry_can_be_switched_off() {
        let p = RetryPolicy {
            retry_transient_idempotent: false,
            ..RetryPolicy::default()
        };
        assert_eq!(
            p.decide(Failure::Status(503), &Method::GET, None, 1, Duration::ZERO),
            Decision::Return
        );
        assert!(matches!(
            p.decide(Failure::Status(429), &Method::GET, None, 1, Duration::ZERO),
            Decision::Retry(_)
        ));
    }
}
