use crate::delta::index::{is_named_node_bound, is_term_bound};
use crate::index::IndexComponents;
use deltalake::kernel::{Add, EagerSnapshot};
use deltalake::logstore::LogStoreRef;
use rdf_fusion_common::{BlankNodeMatchingMode, QuadComponent, TriplePattern};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::ActiveGraph;
use std::sync::Arc;

/// Represents an immutable snapshot of the index at a specific Delta commit version.
///
/// This guarantees that readers see a consistent state of the data and the application log version.
/// Note that we assume that no cleanup job (i.e., vacuuming) is cleaning up the files that are
/// referenced by this snapshot.
#[derive(Debug, Clone)]
pub struct DeltaQuadStorageIndexSnapshot {
    /// The encoding used for storing quads.
    storage_encoding: QuadStorageEncoding,
    /// The log store of the index table.
    log_store: LogStoreRef,
    /// The snapshot of the index table.
    snapshot: EagerSnapshot,
    /// The active files of the index table.
    active_files: Arc<Vec<Add>>,
    /// The components of the index.
    components: IndexComponents,
    /// The log version that this snapshot represents.
    log_version: u64,
}

impl DeltaQuadStorageIndexSnapshot {
    /// Creates a new [`DeltaQuadStorageIndexSnapshot`]. The snapshot and the log store are
    /// expected to belong to the same Delta table.
    pub fn new(
        storage_encoding: QuadStorageEncoding,
        snapshot: EagerSnapshot,
        log_store: LogStoreRef,
        active_files: Arc<Vec<Add>>,
        components: IndexComponents,
        log_version: u64,
    ) -> Self {
        Self {
            storage_encoding,
            snapshot,
            active_files,
            log_store,
            components,
            log_version,
        }
    }

    /// Returns the encoding used for storing quads.
    pub fn encoding(&self) -> QuadStorageEncoding {
        self.storage_encoding.clone()
    }

    /// Returns the current version of the quad storage database that this index snapshot reflects.
    pub fn log_transaction_version(&self) -> u64 {
        self.log_version
    }

    /// Returns the underlying delta table snapshot.
    pub fn eager_snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Returns the cached active files for this snapshot.
    pub fn active_files(&self) -> &Arc<Vec<Add>> {
        &self.active_files
    }

    /// Returns the underlying log store.
    pub fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    /// Returns the components of the index.
    pub fn components(&self) -> IndexComponents {
        self.components
    }

    /// Computes the scan score for this index given the active graph and the pattern.
    pub fn compute_scan_score(
        &self,
        active_graph: &ActiveGraph,
        pattern: &TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> usize {
        let graph_bound = matches!(
            active_graph,
            ActiveGraph::DefaultGraph | ActiveGraph::Union(_)
        );
        let subject_bound = is_term_bound(pattern.subject.clone(), blank_node_mode);
        let predicate_bound =
            is_named_node_bound(pattern.predicate.clone(), blank_node_mode);
        let object_bound = is_term_bound(pattern.object.clone(), blank_node_mode);

        let mut score = 0;
        for (i, component) in self.components.inner().iter().enumerate() {
            let is_bound = match component {
                QuadComponent::GraphName => graph_bound,
                QuadComponent::Subject => subject_bound,
                QuadComponent::Predicate => predicate_bound,
                QuadComponent::Object => object_bound,
            };

            // We don't stop after finding a non-bound component, because components that are
            // part of the sort order should still exhibit a better clustering, even though the
            // scan is no longer restricted to a slice of the index.
            if is_bound {
                let position = 3 - i;
                score += 1 << position;
            }
        }
        score
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::DeltaQuadStorage;
    use crate::delta::index::DeltaQuadStorageIndex;
    use crate::index::IndexComponents;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
    use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
    use object_store::memory::InMemory;
    use rdf_fusion_common::{
        GraphName, NamedNode, NamedNodePattern, Quad, TermPattern, TriplePattern,
        Variable,
    };
    use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
    use rdf_fusion_extensions::storage::QuadStorage;
    use tokio::runtime::Handle;
    use url::Url;

    #[tokio::test]
    async fn test_scan_score_fully_bound() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = index.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 15);
    }

    #[tokio::test]
    async fn test_scan_score_longer_prefixes_score_higher() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern_g = TriplePattern {
            subject: variable_term(),
            predicate: variable_named_node(),
            object: variable_term(),
        };
        let score_g = index.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_g,
            BlankNodeMatchingMode::Filter,
        );

        let pattern_gs = TriplePattern {
            subject: bound_term(),
            predicate: variable_named_node(),
            object: variable_term(),
        };
        let score_gs = index.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_gs,
            BlankNodeMatchingMode::Filter,
        );

        let pattern_gsp = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: variable_term(),
        };
        let score_gsp = index.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_gsp,
            BlankNodeMatchingMode::Filter,
        );

        // Assert that a longer continuous prefix strictly increases the score
        assert!(score_gsp > score_gs);
        assert!(score_gs > score_g);
    }

    #[tokio::test]
    async fn test_scan_score_broken_prefix() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern_broken = TriplePattern {
            subject: variable_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = index.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_broken,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 11);
    }

    #[tokio::test]
    async fn test_scan_score_unbound_first_component() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = index.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::AllGraphs,
            &pattern,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 7);
    }

    #[tokio::test]
    async fn test_index_update_with_only_adds() {
        let config = SessionConfig::new().with_target_partitions(1);
        let session_context = SessionContext::new_with_config(config);
        let storage = DeltaQuadStorage::new_in_memory(
            QuadStorageEncodingName::PlainTerm,
            vec![IndexComponents::GSPO],
        )
        .await;

        let transaction = storage
            .begin_transaction(&session_context.state())
            .await
            .unwrap();
        transaction
            .insert(quads_to_plain_term_dataframe(
                &session_context,
                &[
                    Quad::new(
                        NamedNode::new_unchecked("https://my.test/1"),
                        NamedNode::new_unchecked("https://my.test/1"),
                        NamedNode::new_unchecked("https://my.test/1"),
                        GraphName::DefaultGraph,
                    ),
                    Quad::new(
                        NamedNode::new_unchecked("https://my.test/2"),
                        NamedNode::new_unchecked("https://my.test/2"),
                        NamedNode::new_unchecked("https://my.test/2"),
                        GraphName::DefaultGraph,
                    ),
                ],
            ))
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        // Update indexes
        let state = session_context.state();
        storage.optimize(&state).await.unwrap();

        let index = storage.indexes()[0].clone();
        assert_quad_count(session_context, index, 2).await;
    }

    async fn create_test_index(components: IndexComponents) -> DeltaQuadStorageIndex {
        let memory_store = Arc::new(InMemory::new());
        let url = Url::parse("memory://").unwrap();
        let log_store = logstore_with(
            memory_store,
            &url,
            StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
        )
        .unwrap();
        DeltaQuadStorageIndex::try_new(
            QuadStorageEncoding::PlainTerm,
            log_store,
            components,
        )
        .await
        .expect("Failed to create test index")
    }

    fn bound_term() -> TermPattern {
        NamedNode::new_unchecked("http://example.org/bound").into()
    }

    fn variable_term() -> TermPattern {
        Variable::new_unchecked("v").into()
    }

    fn bound_named_node() -> NamedNodePattern {
        NamedNode::new_unchecked("http://example.org/bound").into()
    }

    fn variable_named_node() -> NamedNodePattern {
        Variable::new_unchecked("v").into()
    }

    async fn assert_quad_count(
        session_context: SessionContext,
        index: Arc<DeltaQuadStorageIndex>,
        expected_count: usize,
    ) {
        let index = index.snapshot().await.unwrap();
        let table_provider = DeltaTableProvider::try_new(
            index.eager_snapshot().clone(),
            index.log_store().clone(),
            DeltaScanConfig::default(),
        )
        .unwrap();
        let count = session_context
            .read_table(Arc::new(table_provider))
            .unwrap()
            .count()
            .await
            .unwrap();
        assert_eq!(count, expected_count);
    }
}
