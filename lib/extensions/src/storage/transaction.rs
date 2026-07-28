use crate::storage::snapshot::QuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use rdf_fusion_common::sparql::algebra::GraphTarget;
use rdf_fusion_common::{BlankNode, NamedNode, StorageError};
use std::sync::Arc;

/// Represents a transaction on a [`QuadStorage`](crate::storage::QuadStorage).
#[async_trait]
pub trait QuadStorageTransaction: Send + Sync {
    /// Returns a [`QuadStorageSnapshot`] for the *current* state of this transaction.
    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError>;

    /// Loads the given quads into the storage.
    ///
    /// The [`DataFrame`] already contains a session state.
    async fn insert(&self, quads: DataFrame) -> Result<Option<usize>, StorageError>;

    /// Removes the given quad from the storage.
    async fn remove(&self, quads: DataFrame) -> Result<Option<bool>, StorageError>;

    /// Creates empty named graphs in the storage.
    async fn create_named_graph(
        &self,
        graphs: DataFrame,
    ) -> Result<Option<bool>, StorageError>;

    /// Clears the entire graph.
    async fn clear_graph(&self, graphs: DataFrame) -> Result<(), StorageError>;

    /// Removes a graph from the storage.
    async fn drop_graph(&self, graphs: DataFrame) -> Result<(), StorageError>;

    /// Returns the number of quads in the storage.
    async fn len(&self, state: &SessionState) -> Result<usize, StorageError>;

    /// Commits this transaction.
    async fn commit(self: Box<Self>) -> Result<(), StorageError>;
}

/// Represents a graph target for the quad storage implementation.
pub enum QuadStorageGraphTarget {
    /// A named graph.
    NamedNode(NamedNode),
    /// A blank node within the scope of the RDF store.
    BlankNode(BlankNode),
    /// The default graph.
    DefaultGraph,
    /// All graphs.
    NamedGraphs,
    /// All graphs (named graphs including the default graph).
    AllGraphs,
}

impl From<GraphTarget> for QuadStorageGraphTarget {
    fn from(value: GraphTarget) -> Self {
        match value {
            GraphTarget::NamedNode(nn) => QuadStorageGraphTarget::NamedNode(nn),
            GraphTarget::DefaultGraph => QuadStorageGraphTarget::DefaultGraph,
            GraphTarget::NamedGraphs => QuadStorageGraphTarget::NamedGraphs,
            GraphTarget::AllGraphs => QuadStorageGraphTarget::AllGraphs,
        }
    }
}
