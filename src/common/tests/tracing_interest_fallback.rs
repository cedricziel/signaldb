use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

struct Count(Arc<AtomicUsize>);
impl<S: tracing::Subscriber> Layer<S> for Count {
    fn on_event(&self, _: &tracing::Event<'_>, _: Context<'_, S>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn emit() {
    tracing::info!("callsite shared between a bare thread and a scoped subscriber");
}

#[test]
fn scoped_subscriber_sees_events_at_callsites_first_hit_without_a_subscriber() {
    common::testing::install_global_tracing_fallback();

    let seen = Arc::new(AtomicUsize::new(0));
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Count(seen.clone())));
    // Another test's thread, with no subscriber, reaches the callsite first.
    std::thread::spawn(emit).join().unwrap();
    tracing::dispatcher::with_default(&dispatch, emit);

    assert_eq!(seen.load(Ordering::SeqCst), 1);
}
