use crate::RdfFusionContextView;
use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_model::StorageError;
use rdf_fusion_model::sparql::Update;
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
use std::sync::Arc;

#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait QuadStorage: Send + Sync {
    /// Returns the quad storage encoding.
    fn encoding(&self) -> QuadStorageEncoding;

    /// Returns a reference to the used [ObjectIdMapping].
    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>>;

    /// Returns a list of planners that support planning logical nodes requiring access to the
    /// storage layer.
    ///
    /// # Consistency
    ///
    /// A query plan must often evaluate multiple quad patterns that have access to the same
    /// storage. It is the responsibility of the storage layer to ensure that the quad patterns use
    /// the same snapshot of the storage layer.
    async fn planners(
        &self,
        context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>>;

    /// Loads the given quads into the storage.
    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError>;

    /// Removes the given quad from the storage.
    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError>;

    /// Creates an empty named graph in the storage.
    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError>;

    /// Returns the list of named graphs in the storage.
    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError>;

    /// Returns whether `graph_name` is a named graph in the storage.
    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError>;

    /// Clears the entire storage.
    async fn clear(&self) -> Result<(), StorageError>;

    /// Clears the entire graph.
    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError>;

    /// Removes the entire named graph from the storage.
    async fn drop_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError>;

    /// Returns the number of quads in the storage.
    async fn len(&self) -> Result<usize, StorageError>;

    /// Optimizes the storage (e.g., building indices).
    async fn optimize(&self) -> Result<(), StorageError>;

    /// Validates invariants in the store
    async fn validate(&self) -> Result<(), StorageError>;

    /// Executes a SPARQL [`Update`] operation against this storage.
    async fn execute_update(&self, update: &Update) -> Result<(), StorageError>;
}
