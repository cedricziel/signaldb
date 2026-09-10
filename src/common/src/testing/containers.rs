//! `testcontainers` pulls images lazily on `.start()` and has no built-in
//! pull retry, so a single Docker Hub registry stream error (e.g. "bytes
//! remaining on stream") fails the whole test suite. This wraps `.start()`
//! with a bounded retry that only fires for that specific error class, so a
//! genuinely bad image reference still fails, just after the retry budget.

use std::future::Future;
use std::time::Duration;

use testcontainers_modules::testcontainers::core::error::ClientError;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, ContainerRequest, Image, TestcontainersError,
};

const MAX_ATTEMPTS: u32 = 3;

fn is_retryable(err: &TestcontainersError) -> bool {
    matches!(
        err,
        TestcontainersError::Client(ClientError::PullImage { .. })
    )
}

fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(u64::from(attempt) * 5)
}

async fn retry_start<T, E, Fut, F>(
    is_retryable: impl Fn(&E) -> bool,
    mut attempt_fn: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt: u32 = 1;
    loop {
        match attempt_fn().await {
            Err(err) if attempt < MAX_ATTEMPTS && is_retryable(&err) => {
                tracing::warn!(attempt, error = %err, "container start failed; retrying");
                tokio::time::sleep(backoff(attempt)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Starts a testcontainer built by `build`, retrying up to three times if
/// the start fails on a Docker Hub image-pull error. `build` is a closure
/// rather than a value because `ContainerRequest` is not `Clone`, so a fresh
/// request must be constructed for each attempt.
pub async fn start_container_with_retry<I, T, F>(mut build: F) -> ContainerAsync<I>
where
    I: Image,
    T: Into<ContainerRequest<I>>,
    F: FnMut() -> T,
{
    retry_start(is_retryable, || {
        let request: ContainerRequest<I> = build().into();
        async move { request.start().await }
    })
    .await
    .unwrap_or_else(|err| panic!("container failed to start after {MAX_ATTEMPTS} attempts: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug)]
    struct StubError(bool);

    impl std::fmt::Display for StubError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stub error (retryable={})", self.0)
        }
    }

    fn stub_is_retryable(err: &StubError) -> bool {
        err.0
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_error_fails_on_first_attempt() {
        let attempts = AtomicU32::new(0);
        let result = retry_start(stub_is_retryable, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(StubError(false)) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_error_is_retried_up_to_max_attempts() {
        let attempts = AtomicU32::new(0);
        let result = retry_start(stub_is_retryable, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(StubError(true)) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    #[tokio::test(start_paused = true)]
    async fn succeeds_once_a_retry_stops_failing() {
        let attempts = AtomicU32::new(0);
        let value = retry_start(stub_is_retryable, || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if attempt < 2 {
                    Err(StubError(true))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(value.ok(), Some(42));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
