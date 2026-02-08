//! Async event handler for the TUI.
//!
//! Multiplexes crossterm input events, tick timers, and render timers
//! into a single [`Event`] stream using `tokio::select!`.

use std::time::Duration;

use crossterm::event::{EventStream, KeyEvent};
use futures::StreamExt;

/// Events produced by the [`EventHandler`].
#[derive(Debug, Clone)]
pub enum Event {
    /// A key press from the user.
    Key(KeyEvent),
    /// Periodic tick for data refresh.
    Tick,
    /// Periodic render signal (~30 fps).
    Render,
}

/// Async event handler that merges crossterm, tick, and render streams.
pub struct EventHandler {
    crossterm_stream: EventStream,
    tick_interval: tokio::time::Interval,
    render_interval: tokio::time::Interval,
}

impl EventHandler {
    /// Create a new event handler with the given tick rate.
    ///
    /// Render rate is fixed at ~30 fps (33 ms).
    pub fn new(tick_rate: Duration) -> Self {
        let render_rate = Duration::from_millis(33);
        Self {
            crossterm_stream: EventStream::new(),
            tick_interval: tokio::time::interval(tick_rate),
            render_interval: tokio::time::interval(render_rate),
        }
    }

    /// Wait for the next event asynchronously.
    pub async fn next(&mut self) -> anyhow::Result<Event> {
        loop {
            tokio::select! {
                maybe_event = self.crossterm_stream.next() => {
                    match maybe_event {
                        Some(Ok(crossterm::event::Event::Key(key))) => {
                            // Filter to Press only — avoids duplicate Release/Repeat on Windows
                            if key.kind == crossterm::event::KeyEventKind::Press {
                                return Ok(Event::Key(key));
                            }
                        }
                        Some(Err(e)) => return Err(e.into()),
                        _ => {}
                    }
                }
                _ = self.tick_interval.tick() => {
                    return Ok(Event::Tick);
                }
                _ = self.render_interval.tick() => {
                    return Ok(Event::Render);
                }
            }
        }
    }
}
