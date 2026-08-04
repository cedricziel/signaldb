//! Terminal User Interface module for SignalDB CLI

pub mod action;
pub mod app;
pub mod client;
pub mod components;
pub mod event;
#[allow(dead_code)] // Scaffolded items used by tests and upcoming tasks
pub mod state;
pub mod terminal;
#[allow(dead_code)] // Scaffolded for upcoming widget implementations
pub mod widgets;

#[cfg(test)]
pub mod test_helpers;
