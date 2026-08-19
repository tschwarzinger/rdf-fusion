use crate::block_cache::BlockCache;
use crate::parquet::reader::PreloadedBloomFilters;
use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::context::SessionState;
use datafusion::parquet::file::metadata::ParquetMetaData;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use rdf_fusion_common::StorageError;
use rdf_fusion_common::config::ParquetStorageOptions;
use rdf_fusion_encoding::object_id::ObjectIdDictionary;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tracing::info;
use url::Url;

/// Builder for [`ParquetQuadStorage`].
pub struct ParquetQuadStorageBuilder<'a> {
    url: Url,
    encoding: QuadStorageEncodingName,
    options: ParquetStorageOptions,
    object_store: Option<Arc<dyn ObjectStore>>,
    object_store_registry: Option<&'a dyn ObjectStoreRegistry>,
    cache: Option<Arc<BlockCache>>,
}

impl<'a> ParquetQuadStorageBuilder<'a> {
    /// Creates a new [`ParquetQuadStorageBuilder`].
    pub fn new(url: Url) -> Self {
        Self {
            url,
            encoding: QuadStorageEncodingName::String,
            options: ParquetStorageOptions::default(),
            object_store: None,
            object_store_registry: None,
            cache: None,
        }
    }

    /// Sets the encoding for the storage.
    pub fn with_encoding(mut self, encoding: QuadStorageEncodingName) -> Self {
        self.encoding = encoding;
        self
    }

    /// Sets the options for the Parquet storage.
    pub fn with_options(mut self, options: ParquetStorageOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the object store.
    pub fn with_object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Sets the object store registry to lookup object stores by URL.
    pub fn with_object_store_registry(
        mut self,
        object_store_registry: &'a dyn ObjectStoreRegistry,
    ) -> Self {
        self.object_store_registry = Some(object_store_registry);
        self
    }

    /// Sets the block cache for caching Parquet file reads.
    pub fn with_cache(mut self, cache: Option<Arc<BlockCache>>) -> Self {
        self.cache = cache;
        self
    }

    /// Builds the [`ParquetQuadStorage`].
    pub async fn build(self) -> Result<ParquetQuadStorage, StorageError> {
        let encoding = match self.encoding {
            QuadStorageEncodingName::PlainTerm => QuadStorageEncoding::PlainTerm,
            QuadStorageEncodingName::String => QuadStorageEncoding::String,
            QuadStorageEncodingName::ObjectId => {
                return Err(StorageError::Other(
                    "ObjectId encoding is not supported for Parquet storage".into(),
                ));
            }
        };

        let object_store = if let Some(store) = self.object_store {
            store
        } else if let Some(registry) = self.object_store_registry {
            registry
                .get_store(&self.url)
                .map_err(|e| StorageError::Other(e.to_string().into()))?
        } else {
            return Err(StorageError::Other(
                "Either object_store or object_store_registry must be provided".into(),
            ));
        };

        let cache = if let Some(cache) = self.cache {
            Some(cache)
        } else if self.options.data_cache_enabled {
            Some(Arc::new(BlockCache::new(
                self.options.data_cache_block_size,
                self.options.data_cache_num_blocks,
            )))
        } else {
            None
        };

        let path = Path::from_url_path(self.url.path())
            .map_err(|e| StorageError::Other(e.to_string().into()))?;
        let object_meta = object_store
            .head(&path)
            .await
            .map_err(|e| StorageError::Other(e.to_string().into()))?;

        info!(
            "Loading Parquet metadata and Bloom filters for file {}...",
            self.url
        );
        let (parquet_meta, bloom_filters) =
            crate::parquet::reader::load_parquet_metadata_and_bloom_filters(
                Arc::clone(&object_store),
                path.clone(),
                object_meta.clone(),
            )
            .await
            .map_err(|e| StorageError::Other(e.to_string().into()))?;
        info!(
            "Parquet metadata and Bloom filters loaded for file {}.",
            self.url
        );

        let bloom_filter_cache = PreloadedBloomFilters::new();
        bloom_filter_cache.insert(path, bloom_filters);

        Ok(ParquetQuadStorage {
            url: self.url,
            encoding,
            object_meta,
            parquet_meta,
            bloom_filter_cache,
            options: self.options,
            cache,
        })
    }
}

/// A quad storage that reads from Parquet files.
///
/// The core approach of this storage layer is based on the storing a large triples / quad table
/// in Parquet. The Parquet files themselves are not specialized for RDF. Any off-the-shelve Parquet
/// reader can access them.
///
/// # References
///
/// - [COTTAS](https://link.springer.com/chapter/10.1007/978-3-032-09530-5_18) that discusses the
///   storage of a triple/quad table in Parquet. Note that we do not follow all recommendations of
///   the paper.
#[derive(Clone)]
pub struct ParquetQuadStorage {
    url: Url,
    encoding: QuadStorageEncoding,
    object_meta: ObjectMeta,
    parquet_meta: Arc<ParquetMetaData>,
    bloom_filter_cache: PreloadedBloomFilters,
    options: ParquetStorageOptions,
    cache: Option<Arc<BlockCache>>,
}

impl ParquetQuadStorage {
    /// Creates a [`ParquetQuadStorageBuilder`].
    pub fn builder(url: Url) -> ParquetQuadStorageBuilder<'static> {
        ParquetQuadStorageBuilder::new(url)
    }

    /// Creates a new [`ParquetQuadStorage`].
    pub async fn try_load(
        url: Url,
        encoding: QuadStorageEncodingName,
        object_store: &dyn ObjectStoreRegistry,
    ) -> Result<Self, StorageError> {
        Self::builder(url)
            .with_encoding(encoding)
            .with_object_store_registry(object_store)
            .build()
            .await
    }

    /// Creates a new [`ParquetQuadStorage`] with the given options.
    pub async fn try_load_with_options(
        url: Url,
        encoding: QuadStorageEncodingName,
        object_store: &dyn ObjectStoreRegistry,
        options: ParquetStorageOptions,
    ) -> Result<Self, StorageError> {
        Self::builder(url)
            .with_encoding(encoding)
            .with_object_store_registry(object_store)
            .with_options(options)
            .build()
            .await
    }

    pub fn bloom_filter_cache(&self) -> &PreloadedBloomFilters {
        &self.bloom_filter_cache
    }

    pub fn options(&self) -> &ParquetStorageOptions {
        &self.options
    }

    pub fn cache(&self) -> Option<&Arc<BlockCache>> {
        self.cache.as_ref()
    }
}

impl Debug for ParquetQuadStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorage")
            .field("url", &self.url)
            .field("encoding", &self.encoding)
            .field("metadata", &self.parquet_meta)
            .field("options", &self.options)
            .finish()
    }
}

#[async_trait]
impl QuadStorage for ParquetQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.encoding.clone()
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdDictionary>> {
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(ParquetQuadStorageSnapshot::new(
            self.encoding.clone(),
            self.url.clone(),
            self.object_meta.clone(),
            Arc::clone(&self.parquet_meta),
            self.bloom_filter_cache.clone(),
            self.cache.clone(),
        )))
    }

    async fn begin_transaction(
        &self,
        _state: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        Err(StorageError::Other("Parquet storage is read-only".into()))
    }

    async fn optimize(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self, _state: &SessionState) -> Result<(), StorageError> {
        // TODO: Validate that quads are unique.
        Ok(())
    }
}
