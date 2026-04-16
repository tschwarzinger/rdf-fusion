use crate::delta::DeltaQuadStorage;
use crate::delta::error::DeltaQuadStorageError;
use crate::index::IndexComponents;
use datafusion::common::Result;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_encoding::typed_family::TypedFamilyEncodingRef;
use rdf_fusion_extensions::storage::QuadStorage;
use std::sync::Arc;

/// Builder for the Delta storage.
#[derive(Clone)]
pub struct DeltaQuadStorageBuilder {
    typed_family_encoding: TypedFamilyEncodingRef,
    location: String,
    encoding: QuadStorageEncodingName,
    indexes: Vec<IndexComponents>,
}

impl DeltaQuadStorageBuilder {
    /// Creates a new [`DeltaQuadStorageBuilder`] with the given [`SessionContext`] for
    /// background operations and the [`TypedFamilyEncodingRef`].
    pub fn new(typed_family_encoding: TypedFamilyEncodingRef) -> Self {
        Self {
            typed_family_encoding,
            location: "memory://".to_string(),
            encoding: QuadStorageEncodingName::ObjectId,
            indexes: vec![
                IndexComponents::GSPO,
                IndexComponents::GPOS,
                IndexComponents::GOSP,
            ],
        }
    }

    /// Sets the location of the delta storage.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = location.into();
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

    /// Tries to create the builder.
    pub async fn build(self) -> Result<Arc<dyn QuadStorage>, DeltaQuadStorageError> {
        let result = DeltaQuadStorage::new_at_location(
            self.encoding,
            self.indexes,
            &self.location,
            self.typed_family_encoding,
        )
        .await?;
        Ok(Arc::new(result))
    }
}
