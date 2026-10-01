use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};

/// Install a process-wide fallback `tracing` subscriber so that scoped
/// (thread-default) subscribers reliably receive events in test binaries.
///
/// tracing-core caches an `Interest` per callsite. While only one dispatcher
/// is registered, a callsite first hit on a thread with no default subscriber
/// is cached as `Interest::never` and stays that way until the next
/// dispatcher registration, so a test that installs a scoped subscriber and
/// asserts on captured events or spans silently loses the ones another
/// test's thread touched first. The fallback answers `Interest::sometimes`,
/// which keeps every callsite consulting the current subscriber, while its
/// own `enabled` is `false` so threads without a scoped subscriber pay
/// nothing. Idempotent, but a process has exactly one global default, so
/// unit tests should call this rather than initialising logging themselves.
pub fn install_global_tracing_fallback() {
    let _ = tracing::subscriber::set_global_default(InterestFallback);
}

struct InterestFallback;

impl Subscriber for InterestFallback {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}
