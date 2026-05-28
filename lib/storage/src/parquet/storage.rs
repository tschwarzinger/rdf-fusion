use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::context::SessionState;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStoreExt};
use rdf_fusion_common::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
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

        let reader =
            ParquetObjectReader::new(object_store, path).with_file_size(object_meta.size);
        let options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
            .await
            .map_err(|e| StorageError::Other(e.to_string().into()))?;

        Ok(Self {
            url,
            encoding,
            object_meta,
            parquet_meta: Arc::clone(builder.metadata()),
        })
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

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(ParquetQuadStorageSnapshot::new(
            self.encoding.clone(),
            self.url.clone(),
            self.object_meta.clone(),
            Arc::clone(&self.parquet_meta),
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
