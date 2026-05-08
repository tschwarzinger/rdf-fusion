use crate::delta::DeltaQuadStorage;
use crate::delta::error::DeltaQuadStorageError;
use crate::index::IndexComponents;
use datafusion::execution::SessionState;
use deltalake::logstore::{IORuntime, LogStoreRef, StorageConfig, logstore_with};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use rdf_fusion_encoding::QuadStorageEncodingName;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::info;

/// Indicates whether the storage builder should try to load an existing table.
#[derive(Clone)]
pub enum LoadMode {
    /// Don't load the table, resulting in an error if it already exists.
    NoLoading,
    /// Load the table.
    Load(Box<SessionState>),
}

/// Builder for the Delta storage.
#[derive(Clone)]
pub struct DeltaQuadStorageBuilder {
    load_mode: LoadMode,
    log_store: Option<LogStoreRef>,
    encoding: QuadStorageEncodingName,
    indexes: Vec<IndexComponents>,
    log_max_age: Option<Duration>,
}

impl DeltaQuadStorageBuilder {
    /// Creates a new [`DeltaQuadStorageBuilder`].
    pub fn new() -> Self {
        Self {
            load_mode: LoadMode::NoLoading,
            log_store: None,
            encoding: QuadStorageEncodingName::ObjectId,
            indexes: vec![
                IndexComponents::GSPO,
                IndexComponents::GPOS,
                IndexComponents::GOSP,
            ],
            log_max_age: None,
        }
    }

    /// Sets the load mode.
    pub fn with_load_mode(mut self, load_mode: LoadMode) -> Self {
        self.load_mode = load_mode;
        self
    }
    pub fn with_log_store(mut self, log_store: LogStoreRef) -> Self {
        self.log_store = Some(log_store);
        self
    }

    /// Sets the encoding of the delta storage.
    pub fn with_encoding(mut self, delta_encoding: QuadStorageEncodingName) -> Self {
        self.encoding = delta_encoding;
        self
    }

    /// Sets which indexes the delta storage should use.
    pub fn with_indexes(mut self, indexes: Vec<IndexComponents>) -> Self {
        self.indexes = indexes;
        self
    }

    /// Sets the maximum age of the transaction log before it is refreshed.
    pub fn with_log_max_age(mut self, max_age: Option<Duration>) -> Self {
        self.log_max_age = max_age;
        self
    }

    /// Tries to create the builder.
    pub async fn build(self) -> Result<DeltaQuadStorage, DeltaQuadStorageError> {
        let log_store = self.log_store.unwrap_or_else(|| {
            use object_store::memory::InMemory;
            let store = Arc::new(InMemory::new());
            let table_url = url::Url::parse("memory:///").unwrap();

            logstore_with(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                &table_url,
                StorageConfig::default()
                    .with_io_runtime(IORuntime::RT(Handle::current())),
            )
            .unwrap()
        });

        let prefix_path = Path::from(log_store.root_url().path());
        let mut list_stream = log_store.root_object_store(None).list(Some(&prefix_path));
        let exists = list_stream.next().await.is_some();

        if exists {
            match self.load_mode {
                LoadMode::NoLoading => Err(DeltaQuadStorageError::Other(
                    "Table already exists.".to_string(),
                )),
                LoadMode::Load(session) => {
                    info!(
                        "Location '{}' is not empty. Loading database ...",
                        &log_store.to_uri(&prefix_path)
                    );

                    let result = DeltaQuadStorage::try_load(&session, log_store).await?;
                    result.set_transaction_max_age(self.log_max_age).await;
                    Ok(result)
                }
            }
        } else {
            info!(
                "Location '{}' was empty. Creating new database ...",
                &log_store.to_uri(&prefix_path)
            );

            let result =
                DeltaQuadStorage::new_at_location(self.encoding, self.indexes, log_store)
                    .await?;
            result.set_transaction_max_age(self.log_max_age).await;
            Ok(result)
        }
    }
}

impl Default for DeltaQuadStorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}
