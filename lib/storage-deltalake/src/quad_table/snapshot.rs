use deltalake::kernel::{Add, EagerSnapshot};
use deltalake::logstore::LogStoreRef;
use rdf_fusion_common::{BlankNodeMatchingMode, TriplePattern};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_storage::quad_tables::{QuadTableName, compute_scan_score};
use std::sync::Arc;

/// Represents an immutable snapshot of the quad_table at a specific Delta commit version.
///
/// This guarantees that readers see a consistent state of the data and the application log version.
/// Note that we assume that no cleanup job (i.e., vacuuming) is cleaning up the files that are
/// referenced by this snapshot.
#[derive(Debug, Clone)]
pub struct DeltaQuadsQuadTableSnapshot {
    /// The encoding used for storing quads.
    storage_encoding: QuadStorageEncoding,
    /// The log store of the quad_table table.
    log_store: LogStoreRef,
    /// The snapshot of the quad_table table.
    snapshot: EagerSnapshot,
    /// The active files of the quad_table table.
    active_files: Arc<Vec<Add>>,
    /// The components of the quad_table.
    components: QuadTableName,
    /// The log version that this snapshot represents.
    log_version: u64,
}

impl DeltaQuadsQuadTableSnapshot {
    /// Creates a new [`DeltaQuadsQuadTableSnapshot`]. The snapshot and the log store are
    /// expected to belong to the same Delta table.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_encoding: QuadStorageEncoding,
        snapshot: EagerSnapshot,
        log_store: LogStoreRef,
        active_files: Arc<Vec<Add>>,
        components: QuadTableName,
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

    /// Returns the current version of the quad storage database that this quad_table snapshot reflects.
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

    /// Returns the components of the quad_table.
    pub fn components(&self) -> QuadTableName {
        self.components
    }

    /// Computes the scan score for this quad_table given the active graph and the pattern.
    pub fn compute_scan_score(
        &self,
        active_graph: &ActiveGraph,
        pattern: &TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> usize {
        compute_scan_score(
            self.components.inner(),
            active_graph,
            pattern,
            blank_node_mode,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeltaQuadsStorage;
    use crate::quad_table::DeltaQuadsQuadTable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use deltalake::delta_datafusion::TableProviderBuilder;
    use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
    use object_store::memory::InMemory;
    use rdf_fusion_common::{
        GraphName, NamedNode, NamedNodePattern, Quad, TermPattern, TriplePattern,
        Variable,
    };
    use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_storage::quad_tables::QuadTableName;
    use tokio::runtime::Handle;
    use url::Url;

    #[tokio::test]
    async fn test_scan_score_fully_bound() {
        let quad_table = create_test_quad_table(QuadTableName::GSPO).await;

        let pattern = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = quad_table.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 15);
    }

    #[tokio::test]
    async fn test_scan_score_longer_prefixes_score_higher() {
        let quad_table = create_test_quad_table(QuadTableName::GSPO).await;

        let pattern_g = TriplePattern {
            subject: variable_term(),
            predicate: variable_named_node(),
            object: variable_term(),
        };
        let score_g = quad_table.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_g,
            BlankNodeMatchingMode::Filter,
        );

        let pattern_gs = TriplePattern {
            subject: bound_term(),
            predicate: variable_named_node(),
            object: variable_term(),
        };
        let score_gs = quad_table.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_gs,
            BlankNodeMatchingMode::Filter,
        );

        let pattern_gsp = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: variable_term(),
        };
        let score_gsp = quad_table.snapshot().await.unwrap().compute_scan_score(
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
        let quad_table = create_test_quad_table(QuadTableName::GSPO).await;

        let pattern_broken = TriplePattern {
            subject: variable_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = quad_table.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_broken,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 11);
    }

    #[tokio::test]
    async fn test_scan_score_unbound_first_component() {
        let quad_table = create_test_quad_table(QuadTableName::GSPO).await;

        let pattern = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = quad_table.snapshot().await.unwrap().compute_scan_score(
            &ActiveGraph::AllGraphs,
            &pattern,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 7);
    }

    #[tokio::test]
    async fn test_quad_table_update_with_only_adds() {
        let options = SessionConfig::default().with_target_partitions(1);
        let session_context = SessionContext::new_with_config(options);
        session_context.runtime_env().register_object_store(
            &Url::parse("memory:///").unwrap(),
            Arc::new(InMemory::new()),
        );

        let storage = DeltaQuadsStorage::new_in_memory(
            QuadStorageEncodingName::PlainTerm,
            vec![QuadTableName::GSPO],
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

        // Update quad tables
        let state = session_context.state();
        storage.optimize(&state).await.unwrap();

        let quad_table = Arc::clone(&storage.quad_tables()[0]);
        assert_quad_count(session_context, quad_table, 2).await;
    }

    async fn create_test_quad_table(components: QuadTableName) -> DeltaQuadsQuadTable {
        let memory_store = Arc::new(InMemory::new());
        let url = Url::parse("memory://").unwrap();
        let log_store = logstore_with(
            memory_store,
            &url,
            StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
        )
        .unwrap();
        DeltaQuadsQuadTable::try_new(
            QuadStorageEncoding::PlainTerm,
            log_store,
            components,
        )
        .await
        .expect("Failed to create test quad_table")
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
        quad_table: Arc<DeltaQuadsQuadTable>,
        expected_count: usize,
    ) {
        let quad_table = quad_table.snapshot().await.unwrap();
        let table_provider = TableProviderBuilder::default()
            .with_eager_snapshot(quad_table.eager_snapshot().clone())
            .with_log_store(Arc::clone(quad_table.log_store()))
            .await
            .unwrap();
        let count = session_context
            .read_table(table_provider)
            .unwrap()
            .count()
            .await
            .unwrap();
        assert_eq!(count, expected_count);
    }
}
