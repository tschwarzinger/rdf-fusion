use crate::memory::MemoryState;
use crate::memory::changes::Changes;
use crate::memory::scan::{PatternConstants, plan_quad_pattern_scan};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_common::{
    GraphName, NamedNode, NamedOrBlankNode, QuadPattern, QuadRef, StorageError, Term,
};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
};
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use std::sync::{Arc, LazyLock};

/// The schema of the named graphs (using the plain term encoding).
static GRAPH_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        COL_GRAPH,
        PLAIN_TERM_ENCODING.data_type().clone(),
        true,
    )]))
});

/// A snapshot of a [`MemoryQuadStorage`](super::MemoryQuadStorage).
///
/// The snapshot consists of a committed state and, for snapshots of transactions, the uncommitted
/// changes of the transaction. Both are immutable and shared, so creating a snapshot never copies
/// quads.
#[derive(Debug, Clone)]
pub struct MemoryQuadStorageSnapshot {
    /// The committed state.
    base: Arc<MemoryState>,
    /// The changes on top of [`Self::base`].
    changes: Arc<Changes>,
}

impl MemoryQuadStorageSnapshot {
    pub(super) fn new(base: Arc<MemoryState>, changes: Arc<Changes>) -> Self {
        Self { base, changes }
    }
}

#[async_trait]
impl QuadStorageSnapshot for MemoryQuadStorageSnapshot {
    async fn scan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        Ok(plan_quad_pattern_scan(
            self.clone(),
            pattern,
            projection,
            session_state.config().batch_size(),
        )?)
    }

    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        let table = MemTable::try_new(
            Arc::clone(&GRAPH_SCHEMA),
            vec![vec![self.named_graphs_batch()]],
        )?;
        Ok(table.scan(state, None, &[], None).await?)
    }

    async fn len(&self, _state: &SessionState) -> Result<usize, StorageError> {
        Ok(self.num_quads())
    }
}

impl MemoryQuadStorageSnapshot {
    /// Returns the quads that match `constants`.
    pub(super) fn quads_for_pattern<'a>(
        &'a self,
        constants: &'a PatternConstants,
    ) -> impl Iterator<Item = QuadRef<'a>> + Send + 'a {
        self.changes.quads_for_pattern(
            &self.base,
            constants.subject.as_ref().map(NamedOrBlankNode::as_ref),
            constants.predicate.as_ref().map(NamedNode::as_ref),
            constants.object.as_ref().map(Term::as_ref),
            constants.graph.as_ref().map(GraphName::as_ref),
        )
    }

    /// Returns the number of quads.
    pub(super) fn num_quads(&self) -> usize {
        self.changes.len(&self.base)
    }

    /// Returns all named graphs (sorted) as a [`RecordBatch`] in the plain term encoding.
    fn named_graphs_batch(&self) -> RecordBatch {
        let mut graphs = self.changes.named_graphs(&self.base).collect::<Vec<_>>();
        graphs.sort_by_cached_key(ToString::to_string);

        let mut builder = PlainTermArrayElementBuilder::with_capacity(graphs.len());
        for graph in graphs {
            builder.append_named_or_blank_node(graph);
        }
        RecordBatch::try_new(
            Arc::clone(&GRAPH_SCHEMA),
            vec![builder.finish().into_array_ref()],
        )
        .expect("Schema matches")
    }
}
