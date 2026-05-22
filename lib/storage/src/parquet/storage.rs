use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionState;
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
    table: Arc<ListingTable>,
}

impl ParquetQuadStorage {
    /// Creates a new [`ParquetQuadStorage`].
    pub fn try_new(
        url: Url,
        encoding: QuadStorageEncodingName,
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

        let table_path = ListingTableUrl::parse(&url)?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(
                ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension(".parquet"),
            )
            .with_schema(Arc::clone(encoding.quad_schema().inner()));
        let table = Arc::new(ListingTable::try_new(config)?);

        Ok(Self {
            url,
            encoding,
            table,
        })
    }
}

impl Debug for ParquetQuadStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorage")
            .field("url", &self.url)
            .field("encoding", &self.encoding)
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
            self.url.clone(),
            self.encoding.clone(),
            Arc::clone(&self.table),
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
