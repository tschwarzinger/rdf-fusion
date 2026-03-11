use crate::index::{EncodedQuad, IndexComponents, IndexPermutations};
use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedObjectId};
use crate::memory::planner::MemQuadStorePlanner;
use crate::memory::storage::quad_index::{MemIndexConfiguration, MemQuadIndex};
use crate::memory::storage::snapshot::MemQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::common::internal_err;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::{
    ObjectIdEncodingRef, ObjectIdMapping, ObjectIdMappingError, ObjectIdMappingExtensions,
    ObjectIdSize,
};
use rdf_fusion_encoding::plain_term::{
    PlainTermQuads, PlainTermQuadsBuilder, PlainTermScalar,
};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_model::StorageError;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A memory-based quad storage.
pub struct MemQuadStorage {
    /// The object id encoding.
    encoding: ObjectIdEncodingRef,
    /// The index set
    indexes: Arc<RwLock<IndexPermutations<MemQuadIndex>>>,
}

impl MemQuadStorage {
    /// Creates a new [MemQuadStorage] with the given `object_id_encoding`.
    ///
    pub fn try_new(
        object_id_encoding: ObjectIdEncodingRef,
        batch_size: usize,
    ) -> DFResult<Self> {
        if object_id_encoding.object_id_size() != ObjectIdSize::try_from(4).unwrap() {
            return internal_err!("Only object id size 4 is supported for now.");
        }

        let components = [
            IndexComponents::GSPO,
            IndexComponents::GPOS,
            IndexComponents::GOSP,
        ];
        let indexes = components
            .iter()
            .map(|components| {
                MemQuadIndex::new(MemIndexConfiguration {
                    object_id_encoding: Arc::clone(&object_id_encoding),
                    batch_size,
                    components: *components,
                })
            })
            .collect();
        Ok(Self {
            indexes: Arc::new(RwLock::new(IndexPermutations::new(
                HashSet::new(),
                indexes,
            ))),
            encoding: object_id_encoding,
        })
    }

    /// Creates a snapshot of this storage.
    pub async fn snapshot(&self) -> MemQuadStorageSnapshot {
        MemQuadStorageSnapshot::new(
            Arc::clone(&self.encoding),
            Arc::new(Arc::clone(&self.indexes).read_owned().await),
        )
    }

    /// TODO
    async fn encode_quad(
        &self,
        quad: QuadRef<'_>,
    ) -> Result<EncodedQuad<EncodedObjectId>, ObjectIdMappingError> {
        let graph_name = self
            .encoding
            .mapping()
            .encode_graph_name(quad.graph_name)?;
        let graph_name = EncodedObjectId::from_4_byte_slice(graph_name.as_bytes());

        let subject = self
            .encoding
            .mapping()
            .encode_scalar(&PlainTermScalar::from(quad.subject))?;
        let subject = EncodedObjectId::from_4_byte_slice(subject.as_bytes());

        let predicate = self
            .encoding
            .mapping()
            .encode_scalar(&PlainTermScalar::from(quad.predicate))?;
        let predicate = EncodedObjectId::from_4_byte_slice(predicate.as_bytes());

        let object = self
            .encoding
            .mapping()
            .encode_scalar(&PlainTermScalar::from(quad.object))?;
        let object = EncodedObjectId::from_4_byte_slice(object.as_bytes());

        Ok(EncodedQuad {
            graph_name,
            subject,
            predicate,
            object,
        })
    }

    async fn encode_quads(
        &self,
        quads: &PlainTermQuads,
    ) -> Result<Vec<EncodedQuad<EncodedObjectId>>, ObjectIdMappingError> {
        let graphs = self.encoding.mapping().encode_array(&quads.graphs)?;
        let subjects = self.encoding.mapping().encode_array(&quads.subjects)?;
        let predicates = self.encoding.mapping().encode_array(&quads.predicates)?;
        let objects = self.encoding.mapping().encode_array(&quads.objects)?;

        let mut encoded = Vec::with_capacity(quads.len());
        for i in 0..quads.len() {
            let graph_name = if graphs.is_valid(i) {
                EncodedObjectId::from_4_byte_slice(graphs.value(i))
            } else {
                DEFAULT_GRAPH_ID
            };

            encoded.push(EncodedQuad {
                graph_name,
                subject: EncodedObjectId::from_4_byte_slice(subjects.value(i)),
                predicate: EncodedObjectId::from_4_byte_slice(predicates.value(i)),
                object: EncodedObjectId::from_4_byte_slice(objects.value(i)),
            });
        }

        Ok(encoded)
    }
}

#[async_trait]
impl QuadStorage for MemQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        // Only object id encoding is supported.
        QuadStorageEncoding::ObjectId(Arc::clone(&self.encoding))
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        Some(Arc::clone(self.encoding.mapping()))
    }

    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let snapshot = self.snapshot().await;
        vec![Arc::new(MemQuadStorePlanner::new(snapshot))]
    }

    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        let mut builder = PlainTermQuadsBuilder::new(quads.len());
        for quad in quads {
            builder.append_quad(quad.as_ref());
        }

        let quads = builder.finish();
        let encoded = self.encode_quads(&quads).await?;

        self.indexes.write().await.insert(encoded.as_ref())
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        let encoded = self.encode_quad(quad).await?;
        let count = self.indexes.write().await.remove(&[encoded]);
        Ok(count > 0)
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let object_id = self
            .encoding
            .mapping()
            .encode_scalar(&PlainTermScalar::from(graph_name))?;

        let encoded = EncodedObjectId::try_from(object_id.as_bytes())
            .expect("Object id size checked in try_new.");
        Ok(self.indexes.write().await.insert_named_graph(encoded))
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        Ok(self.snapshot().await.named_graphs()?)
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        Ok(self.snapshot().await.contains_named_graph(graph_name)?)
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.indexes.write().await.clear();
        Ok(())
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        let encoded = match graph_name {
            GraphNameRef::NamedNode(nn) => {
                let oid = self
                    .encoding
                    .mapping()
                    .encode_scalar(&PlainTermScalar::from(nn))?;
                EncodedObjectId::from_4_byte_slice(oid.as_bytes())
            }
            GraphNameRef::BlankNode(bnode) => {
                let oid = self
                    .encoding
                    .mapping()
                    .encode_scalar(&PlainTermScalar::from(bnode))?;
                EncodedObjectId::from_4_byte_slice(oid.as_bytes())
            }
            GraphNameRef::DefaultGraph => DEFAULT_GRAPH_ID,
        };

        self.indexes.write().await.clear_graph(&encoded);
        Ok(())
    }

    async fn drop_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        let Some(encoded) = self
            .encoding
            .mapping()
            .try_get_object_id(&PlainTermScalar::from(graph_name))?
        else {
            return Ok(false);
        };

        let encoded = EncodedObjectId::try_from(encoded.as_bytes())
            .expect("Object id size checked in try_new.");
        Ok(self.indexes.write().await.drop_named_graph(&encoded))
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.snapshot().await.len())
    }

    async fn optimize(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
