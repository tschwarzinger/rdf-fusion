use crate::rdf_files::detect_encoding_from_schema;
use crate::rdf_files::manager::RdfFileManager;
use crate::rdf_files::rdf::RdfFileSourceConfig;
use crate::rdf_files::snapshot::RdfFileQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::execution::SessionState;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::config::RdfFileStorageOptions;
use rdf_fusion_common::{DFResult, GraphName};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::sync::Arc;
use std::sync::RwLock;

/// A quad storage that reads from data dumps.
#[derive(Clone)]
pub struct RdfFileQuadStorage {
    manager: RdfFileManager,
    sources: Arc<RwLock<Vec<(GraphName, RdfFileSourceConfig)>>>,
    encoding: QuadStorageEncoding,
    options: RdfFileStorageOptions,
}

impl RdfFileQuadStorage {
    /// Creates a new [`RdfFileQuadStorage`] with the given sources.
    pub fn new(
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        options: RdfFileStorageOptions,
    ) -> Self {
        Self::new_with_encoding(sources, QuadStorageEncoding::String, options)
    }

    /// Creates a new [`RdfFileQuadStorage`] with the given sources and encoding.
    pub fn new_with_encoding(
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        encoding: QuadStorageEncoding,
        options: RdfFileStorageOptions,
    ) -> Self {
        Self {
            manager: RdfFileManager::new(),
            sources: Arc::new(RwLock::new(sources)),
            encoding,
            options,
        }
    }

    /// Discovers the encoding from the given sources and creates a new [`RdfFileQuadStorage`].
    pub async fn new_with_discover_encoding(
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        options: RdfFileStorageOptions,
        session_state: &SessionState,
    ) -> DFResult<Self> {
        let encoding = Self::discover_encoding(&sources, session_state).await?;
        Ok(Self::new_with_encoding(sources, encoding, options))
    }

    /// Discovers the encoding from the given sources.
    ///
    /// If no sources are provided or if all sources are RDF files, returns [`QuadStorageEncoding::String`].
    /// If Parquet files are present, it uses the first Parquet file to detect the encoding.
    pub async fn discover_encoding(
        sources: &[(GraphName, RdfFileSourceConfig)],
        session_state: &SessionState,
    ) -> DFResult<QuadStorageEncoding> {
        use rdf_fusion_common::RdfFormat;

        for (_, source) in sources {
            if source.format == RdfFormat::Parquet {
                let manager = RdfFileManager::new();
                let options =
                    crate::rdf_files::RdfFileScanOptions::with_format(RdfFormat::Parquet);
                let mem_table = manager
                    .get_scan_plan(source.url.clone(), options, session_state)
                    .await?;
                return detect_encoding_from_schema(&mem_table.schema());
            }
        }

        Ok(QuadStorageEncoding::String)
    }

    /// Adds a source to the storage.
    pub fn add_source(&self, graph_name: GraphName, source: RdfFileSourceConfig) {
        self.sources.write().unwrap().push((graph_name, source));
    }
}

#[async_trait]
impl QuadStorage for RdfFileQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.encoding.clone()
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(RdfFileQuadStorageSnapshot::new(
            self.manager.clone(),
            self.sources.read().unwrap().clone(),
            self.encoding.clone(),
            self.options.clone(),
        )))
    }

    async fn begin_transaction(
        &self,
        _state: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        Err(StorageError::Other("Data dump storage is read-only".into()))
    }

    async fn optimize(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }
}
