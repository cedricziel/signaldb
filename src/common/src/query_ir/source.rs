//! # Extensible signal-source model
//!
//! The IR `from` source references a **registered** signal source, not a
//! hardcoded enum. Logs and traces are the core sources; the registry lets a
//! later change add `metrics`/`profiles` without reshaping the IR document.

use std::collections::BTreeMap;

use super::relation::Grain;

/// A registered signal source and the static facts the validator needs about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDef {
    /// The logical source name as written in a document's `from`.
    pub name: String,
    /// The grain of one raw row scanned from this source.
    pub grain: Grain,
    /// Whether the `extract` stage is legal on this source (log-only in core).
    pub allows_extract: bool,
}

/// The set of signal sources a server accepts in the `from` field.
#[derive(Debug, Clone)]
pub struct SourceRegistry {
    sources: BTreeMap<String, SourceDef>,
}

impl SourceRegistry {
    /// The core registry for this capability: `logs` and `traces`.
    pub fn core() -> Self {
        let mut sources = BTreeMap::new();
        sources.insert(
            "logs".to_string(),
            SourceDef {
                name: "logs".to_string(),
                grain: Grain::Event,
                allows_extract: true,
            },
        );
        sources.insert(
            "traces".to_string(),
            SourceDef {
                name: "traces".to_string(),
                grain: Grain::Span,
                allows_extract: false,
            },
        );
        SourceRegistry { sources }
    }

    /// Register (or replace) a source. Additive — registering a new source
    /// never changes the shape of a previously-valid document.
    pub fn register(&mut self, def: SourceDef) {
        self.sources.insert(def.name.clone(), def);
    }

    /// Resolve a source by name. Returns `None` for an unregistered source so
    /// the caller can raise a clear unknown-source error (not a parse failure).
    pub fn resolve(&self, name: &str) -> Option<&SourceDef> {
        self.sources.get(name)
    }

    /// The registered source names, sorted.
    pub fn names(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::core()
    }
}
