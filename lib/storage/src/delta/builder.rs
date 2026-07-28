use crate::delta::DeltaQuadsStorage;
use crate::delta::error::DeltaQuadsStorageError;
use crate::quad_tables::QuadTableName;
use datafusion::execution::SessionState;
use deltalake::logstore::{IORuntime, LogStoreRef, StorageConfig, logstore_with};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use rdf_fusion_common::config::{RdfFusionOptions, RdfFusionSessionConfigExt};
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
pub struct DeltaQuadsStorageBuilder {
    load_mode: LoadMode,
    log_store: Option<LogStoreRef>,
    options: Option<RdfFusionOptions>,
    encoding: QuadStorageEncodingName,
    quad_tables: Vec<QuadTableName>,
    log_max_age: Option<Duration>,
}

impl DeltaQuadsStorageBuilder {
    /// Creates a new [`DeltaQuadsStorageBuilder`].
    pub fn new() -> Self {
        Self {
            load_mode: LoadMode::NoLoading,
            log_store: None,
            options: None,
            encoding: QuadStorageEncodingName::ObjectId,
            quad_tables: vec![
                QuadTableName::GSPO,
                QuadTableName::GPOS,
                QuadTableName::GOSP,
            ],
            log_max_age: None,
        }
    }

    /// Sets the load mode.
    pub fn with_load_mode(mut self, load_mode: LoadMode) -> Self {
        self.load_mode = load_mode;
        self
    }

    /// Sets the log store
    pub fn with_log_store(mut self, log_store: LogStoreRef) -> Self {
        self.log_store = Some(log_store);
        self
    }

    /// Sets the delta storage options
    pub fn with_options(mut self, options: Option<RdfFusionOptions>) -> Self {
        self.options = options;
        self
    }

    /// Sets the encoding of the delta storage.
    pub fn with_encoding(mut self, delta_encoding: QuadStorageEncodingName) -> Self {
        self.encoding = delta_encoding;
        self
    }

    /// Sets which quad tables the delta storage should use.
    pub fn with_quad_tables(mut self, quad_tables: Vec<QuadTableName>) -> Self {
        self.quad_tables = quad_tables;
        self
    }

    /// Sets the maximum age of the transaction log before it is refreshed.
    pub fn with_log_max_age(mut self, max_age: Option<Duration>) -> Self {
        self.log_max_age = max_age;
        self
    }

    /// Tries to create the builder.
    pub async fn build(self) -> Result<DeltaQuadsStorage, DeltaQuadsStorageError> {
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
        let exists = match list_stream.next().await {
            Some(Ok(_)) => true,
            Some(Err(e)) => return Err(DeltaQuadsStorageError::Other(e.to_string())),
            None => false,
        };

        if exists {
            match self.load_mode {
                LoadMode::NoLoading => Err(DeltaQuadsStorageError::Other(
                    "Table already exists.".to_string(),
                )),
                LoadMode::Load(session) => {
                    info!(
                        "Location '{}' is not empty. Loading database ...",
                        &log_store.to_uri(&prefix_path)
                    );

                    let options = self.options.unwrap_or_default();
                    let result =
                        DeltaQuadsStorage::try_load(&session, &options, log_store)
                            .await?;
                    result.set_transaction_max_age(self.log_max_age).await;
                    Ok(result)
                }
            }
        } else {
            info!(
                "Location '{}' was empty. Creating new database ...",
                &log_store.to_uri(&prefix_path)
            );

            let session = match &self.load_mode {
                LoadMode::Load(session) => Some(session.as_ref()),
                LoadMode::NoLoading => None,
            };
            let options = session
                .map(|s| s.config().rdf_fusion_options_or_from_env())
                .unwrap_or_else(RdfFusionOptions::from_env)?;

            let result = DeltaQuadsStorage::new_at_location(
                &options,
                self.encoding,
                self.quad_tables,
                log_store,
            )
            .await?;
            result.set_transaction_max_age(self.log_max_age).await;
            Ok(result)
        }
    }
}

impl Default for DeltaQuadsStorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}
