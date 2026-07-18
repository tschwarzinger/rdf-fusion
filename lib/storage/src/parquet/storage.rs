use crate::parquet::reader::PreloadedBloomFilters;
use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::context::SessionState;
use datafusion::parquet::file::metadata::ParquetMetaData;
use log::info;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStoreExt};
use rdf_fusion_common::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdDictionary;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

/// A quad storage that reads from Parquet files.
#[derive(Clone)]
pub struct ParquetQuadStorage {
    url: Url,
    encoding: QuadStorageEncoding,
    object_meta: ObjectMeta,
    parquet_meta: Arc<ParquetMetaData>,
    bloom_filter_cache: PreloadedBloomFilters,
}

impl ParquetQuadStorage {
    /// Creates a new [`ParquetQuadStorage`].
    pub async fn try_load(
        url: Url,
        encoding: QuadStorageEncodingName,
        object_store: &dyn ObjectStoreRegistry,
    ) -> Result<Self, StorageError> {
        let encoding = match encoding {
            QuadStorageEncodingName::PlainTerm => QuadStorageEncoding::PlainTerm,
            QuadStorageEncodingName::String => QuadStorageEncoding::String,
            QuadStorageEncodingName::ObjectId => {
                return Err(StorageError::Other(
                    "ObjectId encoding is not supported for Parquet storage".into(),
                ));
            }
        };

        let object_store = object_store
            .get_store(&url)
            .map_err(|e| StorageError::Other(e.to_string().into()))?;
        let path = Path::from_url_path(url.path())
            .map_err(|e| StorageError::Other(e.to_string().into()))?;
        let object_meta = object_store
            .head(&path)
            .await
            .map_err(|e| StorageError::Other(e.to_string().into()))?;

        info!("Loading Parquet metadata and Bloom filters for file {url}...");
        let (parquet_meta, bloom_filters) =
            crate::parquet::reader::load_parquet_metadata_and_bloom_filters(
                Arc::clone(&object_store),
                path.clone(),
                object_meta.clone(),
            )
            .await
            .map_err(|e| StorageError::Other(e.to_string().into()))?;
        info!("Parquet metadata and Bloom filters loaded for file {url}.");

        let bloom_filter_cache = PreloadedBloomFilters::new();
        bloom_filter_cache.insert(path, bloom_filters);

        Ok(Self {
            url,
            encoding,
            object_meta,
            parquet_meta,
            bloom_filter_cache,
        })
    }

    pub fn bloom_filter_cache(&self) -> &PreloadedBloomFilters {
        &self.bloom_filter_cache
    }
}

impl Debug for ParquetQuadStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorage")
            .field("url", &self.url)
            .field("encoding", &self.encoding)
            .field("metadata", &self.parquet_meta)
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
