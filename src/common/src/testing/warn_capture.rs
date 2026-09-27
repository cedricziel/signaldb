use std::sync::{Arc, Mutex};

use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

/// Collects the messages of WARN events containing a needle, on the current
/// thread, for as long as the returned guard lives.
#[derive(Clone)]
pub struct WarnCapture {
    needle: &'static str,
    messages: Arc<Mutex<Vec<String>>>,
}

impl WarnCapture {
    pub fn install(needle: &'static str) -> (Self, tracing::subscriber::DefaultGuard) {
        super::install_global_tracing_fallback();
        let capture = Self {
            needle,
            messages: Arc::default(),
        };
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        (capture, tracing::subscriber::set_default(subscriber))
    }

    pub fn messages(&self) -> Vec<String> {
        self.messages.lock().unwrap().clone()
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for WarnCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        if visitor.0.contains(self.needle) {
            self.messages.lock().unwrap().push(visitor.0);
        }
    }
}
