//! Library surface for the SignalDB CLI.
//!
//! The binary (`main.rs`) is a thin wrapper over [`commands::Cli`]. The command
//! tree is exposed here so tests — notably the cross-surface query-parity test
//! in `tests-integration` — can introspect it via clap's `CommandFactory`
//! without shelling out to the built binary.

pub mod commands;
pub mod retry;
// `tui` is an internal implementation detail of `commands`; it is not part of
// the library's public surface, so its types stay private (avoids exposing the
// whole widget tree to lints and consumers).
mod tui;
