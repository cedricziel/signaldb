//! A file-backed SQLite catalog whose lifetime a test controls.
//!
//! # Why not a named in-memory database
//!
//! `sqlite:file:name?mode=memory&cache=shared` exists only while some
//! connection to it is open. That makes it unusable for any test where one
//! `CatalogManager` writes and another reads, because each manager builds
//! its own `sqlx` pool: when the writing manager drops, its pool closes in
//! a background task and the database goes with it.
//!
//! Holding a second manager open for the whole test does **not** fix this.
//! `sqlx` pools are lazy and reap idle connections, so a held
//! `CatalogManager` is not a held *connection*. A probe of exactly that
//! shape — pin a manager, create a namespace through a second manager,
//! drop it, wait 500ms — reads back an empty catalog every time.
//!
//! Backing the catalog with a real file removes the dependency on
//! connection lifetime entirely: the database lives as long as the
//! [`TempCatalog`] value, which the test owns.

use tempfile::TempDir;

/// A SQLite catalog backed by a file in a temporary directory.
///
/// Keep the value alive for as long as the catalog is needed — dropping it
/// removes the directory and the database with it.
pub struct TempCatalog {
    _dir: TempDir,
    uri: String,
}

impl TempCatalog {
    /// Create a fresh catalog in a new temporary directory.
    ///
    /// # Panics
    ///
    /// Panics if the temporary directory cannot be created, which in a test
    /// means the environment is unusable.
    pub fn new() -> Self {
        let dir = TempDir::new().expect("failed to create a temporary directory for the catalog");
        // `sqlite:file:` is the file-URI form `create_catalog_with_config`
        // recognizes; it appends `mode=rwc` itself so the database is created.
        let uri = format!("sqlite:file:{}/catalog.db", dir.path().display());
        Self { _dir: dir, uri }
    }

    /// The catalog DSN, suitable for `config.schema.catalog_uri`.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl Default for TempCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the named in-memory DSN cannot provide: data written
    /// through one `CatalogManager` is still readable after that manager is
    /// dropped. This is the exact shape of the provisioning tests, and it
    /// fails against `mode=memory&cache=shared` however long a second
    /// manager is held open.
    #[tokio::test]
    async fn survives_the_writing_manager_being_dropped() {
        let catalog = TempCatalog::new();
        let mut config = crate::config::Configuration::default();
        config.schema.catalog_uri = catalog.uri().to_string();

        {
            let writer = crate::CatalogManager::new(config.clone()).await.unwrap();
            let namespace = writer.build_namespace("acme", "production").unwrap();
            writer
                .catalog()
                .create_namespace(&namespace, None)
                .await
                .unwrap();
        }

        // Let any background pool close run, as a loaded CI runner would.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let reader = crate::CatalogManager::new(config).await.unwrap();
        let namespaces = reader.catalog().list_namespaces(None).await.unwrap();
        assert!(
            !namespaces.is_empty(),
            "a file-backed catalog must outlive the manager that wrote to it"
        );
    }
}
